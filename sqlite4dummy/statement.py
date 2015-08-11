#!/usr/bin/env python
# -*- coding: utf-8 -*-

_CREATE_TABLE_TEMPLATE = \
"""
%s
(
%s%s
)
"""
class Statement():
    pass


class CreateTable():
    temp = _CREATE_TABLE_TEMPLATE
    def __init__(self, table):
        """Generate 'CREATE TABLE' SQL statement.
        
        Example::
        
            CREATE TABLE table_name
            (
                column_name1 dtype1 CONSTRAINS,
                column_name2 dtype2 CONSTRAINS,
                PRIMARY KEY (column, ...),
                FOREIGN KEY (table_column, ...)
            )
            
        **中文文档**
        
        用于根据Table的Schema创建CREATE TABLE的SQL语句。
        """
        clause_CREATE_TABLE = "CREATE TABLE %s" % table.table_name
        clause_DATATYPE = "\t" + ",\n\t".join(
            [self._column_param(column) for column in table]
            )
            
        if len(table.primary_key_columns) == 0:
            clause_PRIMARY_KEY = ""
        else:
            clause_PRIMARY_KEY = ",\n\tPRIMARY KEY (%s)" % ", ".join(
                                                table.primary_key_columns)

        self.sql = self.temp % (clause_CREATE_TABLE,
                           clause_DATATYPE,
                           clause_PRIMARY_KEY,)

    def _column_param(self, column):
        """Generate the definition part of 'CREATE TABLE (...)' SQL command
        by column name, data type, constrains.
        
        Example::
        
            movie_id TEXT
            title TEXT DEFAULT 'unknown_title'
            length INTEGER DEFAULT -1
            release_date DATE NOT NULL
        """
        column_name_part = column.column_name
        data_type_part = column.data_type.sqlite_name
        
        if column.nullable == True:
            nullable_part = None
        else:
            nullable_part = "NOT NULL"
            
        if column.default == None:
            default_part = None
        else:
            default_part = "DEFAULT %s" % column.to_sql_param(column.default)
            
        parts = [column_name_part, data_type_part, nullable_part, default_part]
        
        return " ".join([i for i in parts if i])

if __name__ == "__main__":
    from sqlite4dummy import *
    import unittest
    import sqlite3
    
    class CreateTableUnittest(unittest.TestCase):
        def test_sql(self):
            employee = Table("employee", MetaData(),
                Column("_id", dtype.TEXT, primary_key=True),
                Column("name", dtype.TEXT, nullable=False),
                Column("date_of_birth", dtype.DATE, nullable=False),
                Column("height", dtype.REAL),
                Column("profile", dtype.PICKLETYPE, default={
                                        "role": list(), 
                                        "department": None,
                                        }),
                Column("memo", dtype.TEXT, default="This guy is lazy, no memo."),
                )
            
            create_table = CreateTable(employee)
            
            connect = sqlite3.connect(":memory:", 
                                      detect_types=sqlite3.PARSE_DECLTYPES)
            cursor = connect.cursor()
            cursor.execute(create_table.sql)
            self.assertEqual(
                len(cursor.execute("PRAGMA table_info(employee)").fetchall()),
                6) # 刚创建的表一共有6列
            
    unittest.main()