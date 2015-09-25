#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
English Doc
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

TODO


Chinese Doc (中文文档)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

本模组中的 :class:`SQL_Param` 是处理类 => SQL语句的中间件。用于将用户构造的各种
Select, Insert, Update, Delete, Table, Column, Index对象中的设定, 转化为对应的
SQL参数。


class, method, func, exception
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

try:
    from sqlite4dummy.pycompatible import _str_type
except:
    from .pycompatible import _str_type

class SQL_Param():
    """SQL_Param represent a Where clause parameter in Select statement.
    
    A comparison operation of a Column object and a value results a 
    SQL_Param object.
    
    For example:
    
        >>> Column("height", data_type=dtype.INTEGER) >= 100
        SQL_Param("height >= 100")
         
        >>> Column("date_of_birth", data_type-dtype.DATE).between(
            date(2000, 1, 1), date(2001, 1, 1))
        SQL_Param("date_of_birth BETWEEN '2000-01-01' AND '2001-01-01'")

    :param param: Sql parameter.
    :type param: string
    
    :param column_name: Column name
    :type column_name: string
    
    :param table_name: Table name
    :type table_name: string
    
    :param func_name: Sql function name
    :type func_name: string
    
    :param sql_name: Sql command name
    :type sql_name: string
    
    :param dtype: Data type
    :type dtype: :class:`data type<sqlite4dummy.dtype.BaseDataType>`
    
    **中文文档**
    
    SQL_Param是一个SQL参数的语法构造器。接受的输入参数中处理在SQL中的语句以外,
    额外包含了列名, 表名, 函数名, SQL命令名, 数据类型等参数。这些参数有可能在
    后续的处理中会被用到。
    """
    def __init__(self, param, 
                column_name=None, table_name=None, 
                func_name=None, sql_name=None,
                dtype=None):
        self.param = param
        self.column_name = column_name
        self.table_name = table_name
        self.func_name = func_name
        self.sql_name = sql_name
        self.dtype = dtype

_sql_value_error_message = ("Input has to be list of sqlite4dummy.sql.SQL_Param "
                            "object. Your is {0}")

def and_(*clauses):
    """AND join list of where clause criterions
    """
    try:
        return SQL_Param("(%s)" % " AND ".join([i.param for i in clauses]))
    except AttributeError:
        raise ValueError(_sql_value_error_message.format(repr(clauses)))

def or_(*clauses):
    """OR join list of where clause criterions
    """
    try:
        return SQL_Param("(%s)" % " OR ".join([i.param for i in clauses]))
    except AttributeError:
        raise ValueError(_sql_value_error_message.format(repr(clauses)))
    
def asc(column):
    """sort results by column_name in ascending order
    """
    if isinstance(column, _str_type):
        return SQL_Param("%s ASC" % column, sql_name="ASC")
    else:
        return SQL_Param("%s ASC" % column.full_name,
                          column_name=column.column_name,
                          table_name=column.table_name,
                          sql_name="ASC")

def desc(column):
    """sort results by column_name in descending order
    """
    if isinstance(column, _str_type):
        return SQL_Param("%s DESC" % column, sql_name="DESC")
    else:
        return SQL_Param("%s DESC" % column.full_name,
                          column_name=column.column_name,
                          table_name=column.table_name,
                          sql_name="DESC")
        
if __name__ == "__main__":
    import unittest
    
    class SQLUnittest(unittest.TestCase):
        def test_and(self):
            self.assertRaises(ValueError, and_, "test")
            self.assertEqual(
                and_(SQL_Param("col1 >= 0"), SQL_Param("col2 <= 1")).param,
                "(col1 >= 0 AND col2 <= 1)")
                
        def test_or(self):
            self.assertRaises(ValueError, or_, "test")
            self.assertEqual(
                or_(SQL_Param("col1 >= 0"), SQL_Param("col2 <= 1")).param,
                "(col1 >= 0 OR col2 <= 1)")
            
    unittest.main()