#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlite4dummy.schema import Column, Table
from sqlite4dummy.sql import _SQL_Param

class SqlFunction():
    def __init__(self):
        pass
    
    def count(self, column):
        if isinstance(column, Column):
            return _SQL_Param("COUNT(%s)" % column.column_name, 
                              table_name=column.table_name,
                              func_name="_count")
        else:
            raise Exception

func = SqlFunction()