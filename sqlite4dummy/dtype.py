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
try:
    from sqlite4dummy.pycompatible import PK_PROTOCOL, is_py3 
except:
    from .pycompatible import PK_PROTOCOL, is_py3

from datetime import datetime, date
import binascii
import pickle

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
        """Convert Python object to SQL naive statement.
        
        **中文文档**
        
        将值根据数据类型, 转化成sql可识别的字符形式。
        
        例如::
        
            "Hello World" => "'Hello World'"
        """
        return repr(value)
    
    def from_sql_param(self, text):
        """Convert Sql Quote text to Python object.
        
        **中文文档**
        
        根据sql中的字符串, 根据数据类型, 解析出原始值。
        
        例如::
        
            "'2010-01-01'" => date(2010, 1, 1)
        """
        if text == None:
            return None
        return eval(text)
    
# sqlite3 built-in data type

class TEXT(BaseDataType):
    """UTF-8 string type.
    """
    sqlite_name = "TEXT"

    def to_sql_param(self, str_):
        """Convert Python object to SQL naive statement.
        """
        return "'%s'" % str_.replace("'", "''").replace('"', '\"')

    def from_sql_param(self, text):
        if text is None:
            return None
        else:
            return text[1:-1].replace("''", "'")
        
class INTEGER(BaseDataType):
    """Integer type.
    """
    sqlite_name = "INTEGER"

    def to_sql_param(self, int_):
        """Convert Python object to SQL naive statement.
        """
        return str(int_)

    def from_sql_param(self, text):
        if text is None:
            return None
        else:
            return int(text)
        
class REAL(BaseDataType):
    """Float type.
    """
    sqlite_name = "REAL"

    def to_sql_param(self, float_):
        """Convert Python object to SQL naive statement.
        """
        return str(float_)

    def from_sql_param(self, text):
        """Convert Sql Quote text to Python object.
        """
        if text is None:
            return None
        else:
            return float(text)
        
class BLOB(BaseDataType):
    """Binary type.
    """
    sqlite_name = "BLOB"

    def to_sql_param(self, bytes_):
        """Convert Python object to SQL naive statement.
        """
        return "X'%s'" % binascii.hexlify(bytes_).decode("utf-8")

    def from_sql_param(self, text):
        """Convert Sql Quote text to Python object.
        """
        if text is None:
            return None
        return binascii.unhexlify(text[2:-1])

class DATE(BaseDataType):
    """datetime.date type.
    """
    sqlite_name = "DATE"
    
    def to_sql_param(self, date_):
        """Convert Python object to SQL naive statement.
        """
        return "'%s'" % str(date_)
    
    def from_sql_param(self, text):
        """Convert Sql Quote text to Python object.
        """
        if text is None:
            return None
        return datetime.strptime(text, "'%Y-%m-%d'").date() 
    
class DATETIME(BaseDataType):
    """datetime.datetime type
    """
    sqlite_name = "TIMESTAMP"

    def to_sql_param(self, datetime_):
        """Convert Python object to SQL naive statement.
        """
        return "'%s'" % str(datetime_)

    def from_sql_param(self, text):
        """Convert Sql Quote text to Python object.
        """
        if text is None:
            return None
        try:
            return datetime.strptime(text, "'%Y-%m-%d %H:%M:%S'")
        except:
            return datetime.strptime(text, "'%Y-%m-%d %H:%M:%S.%f'")
    
class PICKLETYPE(BaseDataType):
    """Any picklable Python objects. 
    
    Use pickle.dumps(), pickle.loads() as database IO interface.

    **中文文档**

    任何可被pickle序列化的Python对象。
    """
    sqlite_name = "BLOB"

    def to_sql_param(self, py_obj):
        """Convert python object to SQL naive statement.
        """
        return "X'%s'" % binascii.hexlify(pickle.dumps(py_obj, protocol=PK_PROTOCOL)).decode("utf-8")

    def from_sql_param(self, text):
        """Convert Sql Quote text to Python object.
        """
        if text is None:
            return None
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
            
            注意!! 该测试在Python2中无法通过！ 因为Python2中对于Blob的列, 无法
            正确地使用 SELECT * FROM table WHERE column = X'80025d71002'; 这样
            的形式去匹配。所以在Python2中PickleType只能被读写, 不能被WHERE筛选。
            但可以使用NOT NULL关键字。
            """
            connect = sqlite3.connect(
                ":memory:", detect_types=sqlite3.PARSE_DECLTYPES)
            connect.text_factory = str
            cursor = connect.cursor()
            
            # Define all default values
            id_ = 1
            int_ = 1024
            float_ = 3.14
            str_ = r"""\/!@#$%^&*()_+-=~`|[]{}><,.'"?"""
            bytes_ = "Hello World".encode("utf-8")
            date_ = date(2000, 1, 1)
            datetime_ = datetime(2015, 10, 1, 18, 30, 0)
            py_obj = [1, 2, 3]
            pickle_ = pickle.dumps(py_obj, protocol=PK_PROTOCOL)
            
            record = (id_,
                int_, float_, str_, bytes_, date_, datetime_, pickle_,
                None, None, None, None, None, None, None,
            )
            
            # create table
            create_sql = \
            """
            CREATE TABLE test
            (
                id_ INTEGER PRIMARY KEY NOT NULL,
                
                _int INTEGER DEFAULT %s,
                _float REAL DEFAULT %s,
                _str TEXT DEFAULT %s,
                _bytes BLOB DEFAULT %s,
                _date DATE DEFAULT %s,
                _datetime TIMESTAMP DEFAULT %s,
                _pickle BLOB DEFAULT %s,
                
                _int_null INTEGER,
                _float_null REAL,
                _str_null TEXT,
                _bytes_null BLOB,
                _date_null DATE,
                _datetime_null TIMESTAMP,
                _pickle_null BLOB
            )
            """ % (
                dtype.INTEGER.to_sql_param(int_),
                dtype.REAL.to_sql_param(float_),
                dtype.TEXT.to_sql_param(str_),
                dtype.BLOB.to_sql_param(bytes_),
                dtype.DATE.to_sql_param(date_),
                dtype.DATETIME.to_sql_param(datetime_),
                dtype.PICKLETYPE.to_sql_param(py_obj),
            )
            
            cursor.execute(create_sql)
            
            # insert test data, utilize the default value
            insert_sql = "INSERT INTO test VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
            cursor.execute(insert_sql, record)
            
            # 用Select选择, 看to_sql_param方法使能能将值正确的转换成sql语句
            select_sql = \
            """
            SELECT * FROM test
            WHERE
                _int = ?
                AND _float = ?
                AND _str = ?
                AND _bytes = ?
                AND _date = ?
                AND _datetime = ?
                AND _pickle = ?
            """ 

            # 该条无法再Python2中通过测试, 原因在本方法的docstring中说明了
            self.assertTupleEqual(
                cursor.execute(
                    select_sql, 
                    (int_, float_, str_, bytes_, date_, datetime_, pickle_),
                ).fetchone(), 
                record,
            )
             
            # 测试是否能从metadata中的DEFAULT值的字符串形式获得原始的default值
            res = list(cursor.execute("PRAGMA table_info(test)"))

            self.assertEqual(dtype.INTEGER.from_sql_param(res[0][4]), None)
             
            self.assertEqual(dtype.INTEGER.from_sql_param(res[1][4]), int_)
            self.assertEqual(dtype.REAL.from_sql_param(res[2][4]), float_)
            self.assertEqual(dtype.TEXT.from_sql_param(res[3][4]), str_)
            self.assertEqual(dtype.BLOB.from_sql_param(res[4][4]), bytes_)
            self.assertEqual(dtype.DATE.from_sql_param(res[5][4]), date_)
            self.assertEqual(dtype.DATETIME.from_sql_param(res[6][4]), datetime_)
            self.assertEqual(dtype.PICKLETYPE.from_sql_param(res[7][4]), py_obj)
             
            self.assertEqual(dtype.INTEGER.from_sql_param(res[8][4]), None)
            self.assertEqual(dtype.REAL.from_sql_param(res[9][4]), None)
            self.assertEqual(dtype.TEXT.from_sql_param(res[10][4]), None)
            self.assertEqual(dtype.BLOB.from_sql_param(res[11][4]), None)
            self.assertEqual(dtype.DATE.from_sql_param(res[12][4]), None)
            self.assertEqual(dtype.DATETIME.from_sql_param(res[13][4]), None)
            self.assertEqual(dtype.PICKLETYPE.from_sql_param(res[14][4]), None)
            
    unittest.main()
    