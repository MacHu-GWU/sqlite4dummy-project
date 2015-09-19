#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
本测试模块用于测试与Update有关的功能。

**关于Insert or update和Upsert的一些探讨**

如果不想看具体分析, 直接看结论: 在关系数据库中, upsert意义不大, 而insert or
update更有应用价值。

INSERT AND UPDATE
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

尝试插入一条记录, 如果字段中包括Primary Key, 那么可能出现Integrity Error, 一旦
发生冲突, 则使用WHERE主键Key定位到条目, 然后Update其他字段。

    INSERT INTO test
    (_id, content)
    VALUES 
    (1, 'hello world')

conflict!

    UPDATE test
    SET content = 'hello world'
    WHERE _id = 1

如果字段中不包括Primary Key, 则肯定能Insert成功。

Upsert
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

在关系数据库的条件下, upsert仅对定义了所有的主键的update有效。


class, method, func, exception
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

from sqlite4dummy import *
from datetime import datetime, date
from pprint import pprint as ppt
import sqlalchemy
import unittest
import time
import random

class UpdateSqlUnittest(unittest.TestCase):
    """Unittest of :class:`sqlite4dummy.schema.Update`.
    """
    def setUp(self):
        self.metadata = MetaData()
        self.table = Table("test", self.metadata, 
            Column("_id", dtype.INTEGER, primary_key=True),
            Column("_int", dtype.INTEGER),
            Column("_real", dtype.REAL),
            Column("_text", dtype.TEXT),
            Column("_date", dtype.DATE),
            Column("_datetime", dtype.DATETIME),
            Column("_blob", dtype.BLOB),
            Column("_pickle", dtype.PICKLETYPE),
            )
        self.engine = Sqlite3Engine(":memory:")
        self.metadata.create_all(self.engine)
        
        ins = self.table.insert()
        self.engine.insert_record(ins, 
            (1, 100, 3.14, "abc", date(2000, 1, 1), datetime(2014, 7, 15),
             b"8e01ad49", [1, 2, 3]))
        
    def test_sql(self):
        """测试Update对象是否能正确地构造出SQL语句。
        """
        t = self.table
        upd = t.update().\
            values(_int=200, _real=t.c._int + t.c._real).\
            where(t.c._id == 1, t.c._text == "abc")
        print("{:=^100}".format("update sql"))
        print(upd.sql)
        
    def test_update(self):
        """测试engine.update()的功能。
        """
        t = self.table
        upd = t.update().\
            values(_int=200, 
                   _real=t.c._int + t.c._real, 
                   _text=r"""a\b"x"y'z'b/c""").\
            where(t.c._id == 1, t.c._text == "abc")
        self.engine.update(upd)
        row = list(self.engine.select_row(Select(t.all)))[0]
        self.assertEqual(row._int, 200)
        self.assertEqual(row._real, 103.14)
        self.assertEqual(row._text, r"""a\b"x"y'z'b/c""")        
    
    def test_insdate_many_record(self):
        """测试智能Insert或Update功能在与record协作的情况下是否能够正常运行。
        """
        t = self.table
        ins = t.insert()
        records = [
            (1, 200, 103.14, r"""a\b"x"y'z'b/c""", date(1999, 6, 6), 
             datetime(2050, 12, 31), b"12345678", {"a": 1, "b": 2}),
            (2, 200, 1.414, "xyz", date(2000, 1, 1), datetime(2014, 7, 15),
             b"8e01ad49", [1, 2, 3]),
            (2, 300, 91.8, "ijk", date(2000, 1, 1), datetime(2014, 7, 15),
             b"8e01ad49", [1, 2, 3]),
            ]
        self.engine.insdate_many_record(ins, records)
        rows = list(self.engine.select_row(Select(t.all)))
        self.assertEqual(rows[0]._int, 200)
        self.assertEqual(rows[0]._real, 103.14)
        self.assertEqual(rows[0]._text, r"""a\b"x"y'z'b/c""")  
        
        self.assertEqual(rows[1]._int, 300)
        self.assertEqual(rows[1]._real, 91.8)
        self.assertEqual(rows[1]._text, "ijk")
        
    def test_insdate_many_row(self):
        """测试智能Insert或Update功能在与Row协作的情况下是否能够正常运行。
        """
        t = self.table
        ins = t.insert()
        rows = [
            Row(
            ("_id", "_int", "_real", "_text", "_date", "_datetime", "_blob", 
             "_pickle"),
            (1, 200, 103.14, r"""a\b"x"y'z'b/c""", date(1999, 6, 6), 
             datetime(2050, 12, 31), b"12345678", {"a": 1, "b": 2}),
            ),

            Row(
            ("_id", "_int", "_real", "_text", "_date", "_datetime", "_blob", 
             "_pickle"),
            (2, 200, 1.414, "xyz", date(2000, 1, 1), datetime(2014, 7, 15),
             b"8e01ad49", [1, 2, 3]),
            ),
                
            Row(
            ("_id", "_int", "_real", "_text", "_date", "_datetime", "_blob", 
             "_pickle"),
            (2, 300, 91.8, "ijk", date(2000, 1, 1), datetime(2014, 7, 15),
             b"8e01ad49", [1, 2, 3]),
            ),
            ]
        self.engine.insdate_many_row(ins, rows)
        
        rows = list(self.engine.select_row(Select(t.all)))
        self.assertEqual(rows[0]._int, 200)
        self.assertEqual(rows[0]._real, 103.14)
        self.assertEqual(rows[0]._text, r"""a\b"x"y'z'b/c""")  
        
        self.assertEqual(rows[1]._int, 300)
        self.assertEqual(rows[1]._real, 91.8)
        self.assertEqual(rows[1]._text, "ijk")
        
if __name__ == "__main__":
    unittest.main()