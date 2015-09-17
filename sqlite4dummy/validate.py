#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
To prevent `SQL Injection attack <https://en.wikipedia.org/wiki/SQL_injection>`_
the :class:`Validator` class performs basic check before initializing a 
:class:`Column <sqlite4dummy.schema.Column>` , :class:`Table <sqlite4dummy.schema.Table>`,
:class:`sqlite4dummy.schema.Index` 

class, method, func, exception
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

class ColumnNameError(Exception):
    pass

class TableNameError(Exception):
    pass

class IndexNameError(Exception):
    pass

class Validator():
    """A validator Class to perform validation check.
    """
    def __init__(self):
        pass
    
    def exam_column_name(self, name):
        """Exam column name to avoid sql injection attack.
        
        **中文文档**
        
        为了防止数据库注入攻击, 对列名进行有效性验证。
        """
        _allowed_char = ("ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                         "abcdefghijklmnopqrstuvwxyz"
                         "0123456789_")
        for char in name:
            if char not in _allowed_char:
                raise ColumnNameError(
                    "'%s' is not allowed in column name" % char)
        
        if name[0].isdigit():
            raise ColumnNameError("Column name cannot start with number.")
        
#         if name != name.lower():
#             raise ColumnNameError("Warning: although column name is not "
#                             "case sensitive in sqlite, but we recommend all "
#                             "lower case column name.")
            
    def exam_table_name(self, name):
        """Exam table name to avoid sql injection attack.
        
        **中文文档**
        
        为了防止数据库注入攻击, 对表名进行有效性验证。
        """
        _allowed_char = ("ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                         "abcdefghijklmnopqrstuvwxyz"
                         "0123456789_")
        for char in name:
            if char not in _allowed_char:
                raise TableNameError(
                    "'%s' is not allowed in table name" % char)
        
        if name[0].isdigit():
            raise TableNameError("Table name cannot start with number.")
        
#         if name != name.lower():
#             raise TableNameError("Warning: although table name is not "
#                            "case sensitive in sqlite, but we recommend all "
#                            "lower case table name.")
        
    def exam_index_name(self, name):
        """Exam index name to avoid sql injection attack.
        
        **中文文档**
        
        为了防止数据库注入攻击, 对索引名进行有效性验证。
        """
        _allowed_char = ("ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                         "abcdefghijklmnopqrstuvwxyz"
                         "0123456789_")
        for char in name:
            if char not in _allowed_char:
                raise IndexNameError(
                    "'%s' is not allowed in index name" % char)
        
        if name[0].isdigit():
            raise IndexNameError("Index name cannot start with number.")
        
        if name != name.lower():
            raise IndexNameError("Warning: although index name is not "
                           "case sensitive in sqlite, but we recommend all "
                           "lower case index name.")
                
validator = Validator()

if __name__ == "__main__":
    import unittest
    
    class ValidatorUnittest(unittest.TestCase):
        def test_exam_column_name(self):
            self.assertRaises(ColumnNameError,
                              validator.exam_column_name,
                              "my%")
            self.assertRaises(ColumnNameError,
                              validator.exam_column_name,
                              "007_profile")
            self.assertRaises(ColumnNameError,
                              validator.exam_column_name,
                              "BigColumn")
            validator.exam_column_name("_id")

        def test_exam_table_name(self):
            self.assertRaises(TableNameError,
                              validator.exam_table_name,
                              "my%")
            self.assertRaises(TableNameError,
                              validator.exam_table_name,
                              "007_profile")
            self.assertRaises(TableNameError,
                              validator.exam_table_name,
                              "BigTable")
            validator.exam_table_name("_product")

        def test_exam_index_name(self):
            self.assertRaises(IndexNameError,
                              validator.exam_index_name,
                              "my%")
            self.assertRaises(IndexNameError,
                              validator.exam_index_name,
                              "007_profile")
            self.assertRaises(IndexNameError,
                              validator.exam_index_name,
                              "BigTable")
            validator.exam_index_name("_myindex")
                 
    unittest.main()