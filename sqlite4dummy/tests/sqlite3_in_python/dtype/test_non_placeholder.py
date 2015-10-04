#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
在sqlite3中, Insert的命令看起来是这样的::

    cursor.execute("INSERT INTO VALUES (?,?)", (value1, value2))

?问号在sqlite3 DBAPI中作为placeholder标识符, 用于处理Python对象到SQL
statement的转换。

但是对于CREATE TABLE ... DEFAULT语法, 我们无法使用placeholder。我们必须使用纯
字符串的SQL命令, 手写这一命令。

而我们在查看数据表的metadata时, 需要使用 ``PRAGMA table_info(table-name)``。该
命令返回的列的default值是纯字符串。而要从纯字符串中获取Python中的对象, 我们必须
对所有的基本数据类型定义一些方法, 手动完成转换过程。

本测试对六大基本数据类型定义了 ``to_sql`` 和 ``from_sql`` 方法, 测试在Python2, 3中
通用的数据值和SQL参数的转换方法是否能正确工作。
"""

from __future__ import print_function
from datetime import datetime, date
import unittest
import binascii
import sqlite3
import pickle
import sys

if sys.version_info[0] == 3:
    is_py3 = True
else:
    is_py3 = False
    
def to_sql_INTEGER(int_):
    return int_

def to_sql_REAL(float_):
    return float_

def to_sql_TEXT(str_):
    return "'%s'" % str_.replace("'", "''").replace('"', '\"')

def to_sql_BLOB(bytes_):
    return "X'%s'" % binascii.hexlify(bytes_).decode("utf-8")

def to_sql_DATE(date_):
    return "'%s'" % str(date_)

def to_sql_DATETIME(datetime_):
    return "'%s'" % str(datetime_)

def from_sql_INTEGER(text):
    if text is None:
        return None
    else:
        return int(text)

def from_sql_REAL(text):
    if text is None:
        return None
    else:
        return float(text)

def from_sql_TEXT(text):
    if text is None:
        return None
    else:
        return text[1:-1].replace("''", "'")

def from_sql_BLOB(text):
    if text is None:
        return None
    return binascii.unhexlify(text[2:-1])

def from_sql_DATE(text):
    if text is None:
        return None
    return datetime.strptime(text, "'%Y-%m-%d'").date() 

def from_sql_DATETIME(text):
    if text is None:
        return None
    try:
        return datetime.strptime(text, "'%Y-%m-%d %H:%M:%S'")
    except:
        return datetime.strptime(text, "'%Y-%m-%d %H:%M:%S.%f'")

class Unittest(unittest.TestCase):
    def test_placeholder_for_string(self):
        """Test for ``to_sql`` and ``from_sql`` method.
        """
        connect = sqlite3.connect(
            ":memory:", detect_types=sqlite3.PARSE_DECLTYPES)
        connect.text_factory = str
        cursor = connect.cursor()
        
        create_table_sql = \
        """
        CREATE TABLE test
        (
            _id INTEGER PRIMARY KEY NOT NULL,
            _int INTEGER DEFAULT %s,
            _float REAL DEFAULT %s,
            _str TEXT DEFAULT %s,
            _bytes BLOB DEFAULT %s,
            _date DATE DEFAULT %s,
            _datetime TIMESTAMP DEFAULT %s
        )
        """
        
        int_ = 1024
        float_ = 3.14
        str_ = r"""\/!@#$%^&*()_+-=~`|[]{}><,.'"?"""
        bytes_ = pickle.dumps([1,2,3], protocol=2)
        date_ = date(2000, 1, 1)
        datetime_ = datetime(2015, 10, 1, 18, 30, 0)
        
        create_table_sql = create_table_sql % (
            to_sql_INTEGER(int_),
            to_sql_REAL(float_),
            to_sql_TEXT(str_),
            to_sql_BLOB(bytes_),
            to_sql_DATE(date_),
            to_sql_DATETIME(datetime_),
        )
        
        cursor.execute(create_table_sql)
        
        # 插入一条数据, 除了_id的其他列使用DEFAULT中的设定值
        cursor.execute("INSERT INTO test (_id) VALUES (?)", (1,))
        
        # 取出刚才那条自动生成的数据, 并与之前的默认值对比, 看是否一致
        record = cursor.execute("SELECT * FROM test").fetchone()
        
        self.assertEqual(record[1], int_)
        self.assertEqual(record[2], float_)
        self.assertEqual(record[3], str_)
        # 由于在Python2中, pickle.dumps返回的是str, 所以导致无法相等。
        # 但对于本身就是bytes的, 不会造成任何影响。
#         self.assertEqual(record[4], bytes(bytes_)) 
        self.assertEqual(pickle.loads(record[4]), [1, 2, 3])
        self.assertEqual(record[5], date_)
        self.assertEqual(record[6], datetime_)
        # Expect: (1, 1024, 3.14, '\\/!@#$%^&*()_+-=~`|[]{}><,.\'"?', 
        # datetime.date(2000, 1, 1), datetime.datetime(2015, 10, 1, 18, 30))
#         print(cursor.execute("SELECT * FROM test").fetchone())
         
        default_value = [
            record[4] for record in cursor.execute("PRAGMA table_info(test)")]
 
        self.assertEqual(from_sql_INTEGER(default_value[1]), int_)
        self.assertEqual(from_sql_REAL(default_value[2]), float_)
        self.assertEqual(from_sql_TEXT(default_value[3]), str_)
        self.assertEqual(from_sql_BLOB(default_value[4]), bytes_)
        self.assertEqual(pickle.loads(from_sql_BLOB(default_value[4])), [1, 2, 3])
        self.assertEqual(from_sql_DATE(default_value[5]), date_)
        self.assertEqual(from_sql_DATETIME(default_value[6]), datetime_)
        
        connect.close()
        
if __name__ == "__main__":
    unittest.main()