#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
placeholder是DBAPI中用于占用一个SQL Statement中的值的位置, 然后将该位置根据编译
器对输入的值进行编译。

在CREATE语句中的DEFAULT部分无法使用placeholder。而在
Insert, Update, Replace, Select, Delete中都可以使用placeholder。
"""

from __future__ import print_function
import unittest
import sqlite3

class Unittest(unittest.TestCase):
    def test_placeholder_for_string(self):
        """本例测试了使用sqlite3中的?问号符作为placeholder, 对字符串中的特殊字符
        例如: "\", "/", "%", "'", '"', "\t", "\n"等进行处理的情况。
        
        结论: 能用Place Holder, 尽量使用Place Holder.
        """
        connect = sqlite3.connect(":memory:")
        cursor = connect.cursor()
        
        create_table_sql = \
        """
        CREATE TABLE test (_id INTEGER, _str TEXT DEFAULT 'Hello World!')
        """
        cursor.execute(create_table_sql)
        
        # Insert
        cursor.execute("INSERT INTO test (_id) VALUES (?)", (0,))
        old_text = \
        r"""
        \t
        \n
        a/b
        b\a
        "c"
        'd'
        %s
        %f
        """
        cursor.execute("INSERT INTO test VALUES (?,?)", (1, old_text))
        record = cursor.execute("SELECT * FROM test WHERE _id = 1").fetchone()
        print(record[1]) # old_text
        
        # Update
        new_text = \
        r"""
        "c"
        'd'
        %s
        %f
        \t
        \n
        a/b
        b\a
        """
        cursor.execute("UPDATE test SET _str = ? WHERE _id = ?", (new_text, 1))
        record = cursor.execute("SELECT * FROM test WHERE _id = 1").fetchone()
        print(record[1]) # new_text
        
        # Replace
        cursor.execute("REPLACE INTO test VALUES (?,?)", (2, "Third Record"))
        record = cursor.execute("SELECT * FROM test WHERE _id = 2").fetchone()
        print(record[1]) # Third Record
        
        # Select
        cursor.execute("SELECT * FROM test WHERE _str = ?", (new_text,))
        record = cursor.execute("SELECT * FROM test WHERE _id = 1").fetchone()
        print(record[1]) # new_text
        
        # Delete
        cursor.execute("DELETE FROM test WHERE _str = ?", (new_text,))
        record = cursor.execute("SELECT COUNT(*) FROM (SELECT * FROM test)").fetchone()
        print(record[0]) # After deletion, only 2 records left
        
        connect.close()
        
    def test_special_char_in_sql_statement(self):
        """在Sqlite的Sql语句中, 有两个特殊字符'和"需要特别对待。
        '需要被写成'', "需要被写成\"。但是由于在Sql语句中只有CREATE语句不能用
        placeholder, 所以一般没有大的影响。
        """
        text = r"""\/!@#$%^&*()_+-=~`|[]{}><,.'"?"""
        connect = sqlite3.connect(":memory:")
        cursor = connect.cursor()
        
        create_table_sql = \
        """
        CREATE TABLE test (_id INTEGER, _str TEXT DEFAULT '%s')
        """ % text.replace("'", "''").replace('"', '\"')
        cursor.execute(create_table_sql)
        
        cursor.execute("INSERT INTO test (_id) VALUES (?)", (1,))
        record = cursor.execute("SELECT * FROM test").fetchone()
        print(record[1]) # \/!@#$%^&*()_+-=~`|[]{}><,.'"?
        
        connect.close()
        
if __name__ == "__main__":
    unittest.main()