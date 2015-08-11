#!/usr/bin/env python
# -*- coding: utf-8 -*-

class _SQL_Param():
    """_SQL_Param represent a Where clause parameter in Select statement.
    
    A comparison operation of a Column object and a value results a 
    _SQL_Param object.
    
    For example:
    
        >>> Column("height", data_type=dtype.INTEGER) >= 100
        _SQL_Param("height >= 100")
         
        >>> Column("date_of_birth", data_type-dtype.DATE).between(
            date(2000, 1, 1), date(2001, 1, 1))
        _SQL_Param("date_of_birth BETWEEN '2000-01-01' AND '2001-01-01'")
    """
    def __init__(self, param, table_name=None, func_name=None):
        self.param = param
        self.table_name = table_name
        self.func_name = func_name
        
def and_(*clauses):
    """AND join list of where clause criterions
    """
    return _SQL_Param("(%s)" % " AND ".join([i.param for i in clauses]))

def or_(*clauses):
    """OR join list of where clause criterions
    """
    return _SQL_Param("(%s)" % " OR ".join([i.param for i in clauses]))

def asc(column):
    """sort results by column_name in ascending order
    """
    return _SQL_Param("%s ASC" % column.full_name)

def desc(column):
    """sort results by column_name in descending order
    """
    return _SQL_Param("%s DESC" % column.full_name)