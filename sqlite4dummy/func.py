#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This module implements SQL generic functions like: count, max, min, abs, etc...

For more information about Sqlite understood function, see:
https://www.sqlite.org/lang_corefunc.html

For usage example, go :mod:`unittest page<sqlite4dummy.tests.test_func>`
and read the testcase source code.

class, method, func, exception
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

try:
    from sqlite4dummy.dtype import dtype
except ImportError:
    from .dtype import dtype

try:
    from sqlite4dummy.schema import Column
except ImportError:
    from .schema import Column
    
try:
    from sqlite4dummy.sql import SQL_Param
except ImportError:
    from .sql import SQL_Param

class SqlFunction():
    """
    Implement SQL generic functions.
    
    All method can only take :class:`~sqlite4dummy.schema.Column` object argument.
    
    **中文文档**
    
    实现了Sql中的计算函数。对于所有Sqlite所支持的函数, 请参考
    https://www.sqlite.org/lang_corefunc.html
    """
    def __init__(self):
        pass
    
    def count(self, column):
        """COUNT(column) function.
        """
        if isinstance(column, Column):
            return SQL_Param("COUNT(%s)" % column.column_name,
                              column_name=column.column_name, 
                              table_name=column.table_name,
                              func_name="_count_",
                              dtype=dtype.INTEGER,)
        else:
            raise Exception("func.count()'s argument has to be Column object.")
        
    def max(self, column):
        """MAX(column) function.
        """
        if isinstance(column, Column):
            return SQL_Param("MAX(%s)" % column.column_name,
                              column_name=column.column_name, 
                              table_name=column.table_name,
                              func_name="_max_",
                              dtype=column.data_type,)
        else:
            raise Exception("func.max()'s argument has to be Column object.")

    def min(self, column):
        """MIN(column) function.
        """
        if isinstance(column, Column):
            return SQL_Param("MIN(%s)" % column.column_name,
                              column_name=column.column_name, 
                              table_name=column.table_name,
                              func_name="_min_",
                              dtype=column.data_type,)
        else:
            raise Exception("func.min()'s argument has to be Column object.")

    def abs(self, column):
        """ABS(column) function.
        """
        if isinstance(column, Column):
            return SQL_Param("ABS(%s)" % column.column_name,
                              column_name=column.column_name, 
                              table_name=column.table_name,
                              func_name="_abs_",
                              dtype=column.data_type,)
        else:
            raise Exception("func.abs()'s argument has to be Column object.")
        
    def length(self, column):
        """LENGTH(column) function.
        """
        if isinstance(column, Column):
            return SQL_Param("LENGTH(%s)" % column.column_name,
                              column_name=column.column_name, 
                              table_name=column.table_name,
                              func_name="_length_",
                              dtype=dtype.INTEGER,)
        else:
            raise Exception("func.length()'s argument has to be Column object.")
        
    def lower(self, column):
        """LOWER(column) function.
        """
        if isinstance(column, Column):
            return SQL_Param("LOWER(%s)" % column.column_name,
                              column_name=column.column_name, 
                              table_name=column.table_name,
                              func_name="_lower_",
                              dtype=dtype.TEXT,)
        else:
            raise Exception("func.lower()'s argument has to be Column object.")

    def upper(self, column):
        """UPPER(column) function.
        """
        if isinstance(column, Column):
            return SQL_Param("UPPER(%s)" % column.column_name,
                              column_name=column.column_name, 
                              table_name=column.table_name,
                              func_name="_upper_",
                              dtype=dtype.TEXT,)
        else:
            raise Exception("func.upper()'s argument has to be Column object.")
    
    def round(self, column):
        """ROUND(column) function.
        """
        if isinstance(column, Column):
            return SQL_Param("ROUND(%s)" % column.column_name,
                              column_name=column.column_name, 
                              table_name=column.table_name,
                              func_name="_round_",
                              dtype=dtype.TEXT,)
        else:
            raise Exception("func.upper()'s argument has to be Column object.")
        
func = SqlFunction()