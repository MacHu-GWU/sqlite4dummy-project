#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
本测试模块用于测试与 :class:`sqlite4dummy.schema.Column` 有关的方法


class, method, func, exception
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

from sqlite4dummy import *
from datetime import datetime, date
import unittest

class ColumnUnittest(unittest.TestCase):
    """Unittest of :class:`sqlite4dummy.schema.Column`.
    
    Column的方法的单元测试。
    """
    def test_str_and_repr(self):
        """测试Column的 ``__str__`` 和 ``__repr__`` 方法。
        """
        column = Column("_id", dtype.INTEGER,
                        nullable=False, default=None, primary_key=True)
        self.assertEqual(str(column), "_id")
        self.assertEqual(repr(column),
            "Column('_id', dtype.INTEGER, nullable=False, default=None, primary_key=True)")
        
        column = Column("_text", dtype.TEXT, default="hello world")
        self.assertEqual(str(column), "_text")
        self.assertEqual(repr(column), 
            "Column('_text', dtype.TEXT, nullable=True, default='hello world', primary_key=False)")

        column = Column("_int", dtype.INTEGER, nullable=False)
        self.assertEqual(str(column), "_int")
        self.assertEqual(repr(column), 
            "Column('_int', dtype.INTEGER, nullable=False, default=None, primary_key=False)")

        column = Column("_float", dtype.REAL, nullable=False)
        self.assertEqual(str(column), "_float")
        self.assertEqual(repr(column), 
            "Column('_float', dtype.REAL, nullable=False, default=None, primary_key=False)")

        column = Column("_byte", dtype.BLOB, default=b"8e01ad49")
        self.assertEqual(str(column), "_byte")
        self.assertIn(repr(column), 
            ["Column('_byte', dtype.BLOB, nullable=True, default=b'8e01ad49', primary_key=False)",
             "Column('_byte', dtype.BLOB, nullable=True, default='8e01ad49', primary_key=False)",])
        
        column = Column("_date", dtype.DATE, default=date(2000, 1, 1))
        self.assertEqual(str(column), "_date")
        self.assertEqual(repr(column), 
            "Column('_date', dtype.DATE, nullable=True, default=datetime.date(2000, 1, 1), primary_key=False)")

        column = Column("_datetime", dtype.DATE, default=datetime(2015, 7, 15, 6, 30))
        self.assertEqual(str(column), "_datetime")
        self.assertEqual(repr(column), 
            "Column('_datetime', dtype.DATE, nullable=True, default=datetime.datetime(2015, 7, 15, 6, 30), primary_key=False)")

        column = Column("_pickle", dtype.PICKLETYPE, default=[1, 2, 3])
        self.assertEqual(str(column), "_pickle")
        self.assertEqual(repr(column), 
            "Column('_pickle', dtype.PICKLETYPE, nullable=True, default=[1, 2, 3], primary_key=False)")
        
    def test_comparison_operator(self):
        """测试比较运算符是否能正确产生 :class:`~sqlite4dummy.sql.SQL_Param` 对象,
        并且确保对应的SQL字符串正确。
        """
        c = Column("other_column", dtype.TEXT)
        
        # integer type
        column = Column("_int", dtype.INTEGER)
        t = Table("test", MetaData(), c, column)
        
        self.assertEqual((column > 1).param, "test._int > 1")
        self.assertEqual((column >= 1).param, "test._int >= 1")
        self.assertEqual((column < 1).param, "test._int < 1")
        self.assertEqual((column <= 1).param, "test._int <= 1")
        self.assertEqual((column == 1).param, "test._int = 1")
        self.assertEqual((column != 1).param, "test._int != 1")
        self.assertEqual((column.between(0, 1)).param, 
                         "test._int BETWEEN 0 AND 1")
        self.assertEqual((column.in_([1, 2, 3])).param, "test._int IN (1, 2, 3)")
        
        # text type
        column = Column("_text", dtype.TEXT)
        t = Table("test", MetaData(), c, column)
        
        self.assertEqual((column > "abc").param,
                         "test._text > 'abc'")
        self.assertEqual((column >= "abc").param,
                         "test._text >= 'abc'")
        self.assertEqual((column < "abc").param,
                         "test._text < 'abc'")
        self.assertEqual((column <= "abc").param,
                         "test._text <= 'abc'")
        self.assertEqual((column == "abc").param,
                         "test._text = 'abc'")
        self.assertEqual((column != "abc").param,
                         "test._text != 'abc'")
        self.assertEqual((column.like("%abc%")).param,
                         "test._text LIKE '%abc%'")
        
        # date type
        column = Column("_date", dtype.DATE)
        t = Table("test", MetaData(), c, column)
        
        self.assertEqual((column > date(2000, 1, 1)).param, 
                         "test._date > '2000-01-01'")
        self.assertEqual((column >= date(2000, 1, 1)).param, 
                         "test._date >= '2000-01-01'")
        self.assertEqual((column < date(2000, 1, 1)).param, 
                         "test._date < '2000-01-01'")
        self.assertEqual((column <= date(2000, 1, 1)).param, 
                         "test._date <= '2000-01-01'")
        self.assertEqual((column == date(2000, 1, 1)).param, 
                         "test._date = '2000-01-01'")
        self.assertEqual((column != date(2000, 1, 1)).param, 
                         "test._date != '2000-01-01'")
        self.assertEqual((column.between(date(2000, 1, 1), 
                                         date(2000, 12, 31))).param, 
                         "test._date BETWEEN '2000-01-01' AND '2000-12-31'")
        
        # blob type
        column = Column("_blob", dtype.BLOB)
        t = Table("test", MetaData(), c, column)
        
        self.assertIn(
            (column == b"8e01ad49").param, 
            ["test._blob = X'3865303161643439'", ],
            )
        self.assertEqual((column != b"8e01ad49").param, 
                         "test._blob != X'3865303161643439'")
        
        # pickle type
        column = Column("_pickle", dtype.PICKLETYPE)
        t = Table("test", MetaData(), c, column)
        
        self.assertIn(
            (column == [1, 2, 3]).param, 
            ["test._pickle = X'80035d7100284b014b024b03652e'", # Py3
             "test._pickle = X'80025d7100284b014b024b03652e'"],
            )
        self.assertIn(
            (column != [1, 2, 3]).param, 
            ["test._pickle != X'80035d7100284b014b024b03652e'",
             "test._pickle != X'80025d7100284b014b024b03652e'"],
            )
        
        # None type
        self.assertEqual((column == None).param,
                         "test._pickle IS NULL")
        self.assertEqual((column != None).param,
                         "test._pickle NOT NULL")
        
        # column type, 列与列比较
        column = Column("_this", dtype.TEXT)
        t = Table("test", MetaData(), c, column)
        
        self.assertEqual((column > c).param, "test._this > test.other_column")
        self.assertEqual((column >= c).param, "test._this >= test.other_column")
        self.assertEqual((column < c).param, "test._this < test.other_column")
        self.assertEqual((column <= c).param, "test._this <= test.other_column")
        self.assertEqual((column == c).param, "test._this = test.other_column")
        self.assertEqual((column != c).param, "test._this != test.other_column")
        self.assertEqual((column.between(c, c)).param, 
                         "test._this BETWEEN test.other_column AND test.other_column")
        self.assertEqual((column.between("abc", c)).param, 
                         "test._this BETWEEN 'abc' AND test.other_column")
        self.assertEqual((column.between(c, "abc")).param, 
                         "test._this BETWEEN test.other_column AND 'abc'")
        self.assertEqual((column.between("abc", "xyz")).param, 
                         "test._this BETWEEN 'abc' AND 'xyz'")
    
    def test_calculation_operator(self):
        this = Column("_this", dtype.INTEGER)
        that = Column("_that", dtype.INTEGER)
        t = Table("test", MetaData(), this, that)
        
        self.assertEqual((this + 1).param, "test._this + 1")
        self.assertEqual((this - 1).param, "test._this - 1")
        self.assertEqual((this * 1).param, "test._this * 1")
        self.assertEqual((this / 1).param, "test._this / 1")
        self.assertEqual((+ this).param, "+ test._this")
        self.assertEqual((- this).param, "- test._this")
        
if __name__ == "__main__":
    unittest.main()