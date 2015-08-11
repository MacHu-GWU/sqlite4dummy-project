#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlite4dummy.row import Row
from sqlite4dummy.iterate import grouper_list
from collections import OrderedDict
import sqlite3
import pickle
import sys

is_py2 = (sys.version_info[0] == 2)
if is_py2:
    pk_protocol = 2
else:
    pk_protocol = 3


class PickleTypeConverter():
    """
    
    **中文文档**
    
    用于解决PickleType的读写的数据转换器。
    
    在执行INSERT时, 需要将tuple或row dict转换成sqlite3原生API可接受的tuple, 并
    要保证PickleType正确地被序列化。
    
    在执行Select时, 需要cursor返回的tuple转换成被pickle解码后的Python对象。
    
    在这两个过程中, 需要有所有涉及到的Column的相关信息。我们的做法是使用
    self.table保存所有的MetaData, 然后利用map高性能并行处理函数, 批量转换数据。
    """
    def __init__(self, table):
        self.table = table

    def convert_record(self, record):
        if len(self.table.pickletype_columns):
            new_record = list()
            for column, value in zip(self.table.all, record):
                if value:
                    if column.is_pickletype:
                        new_record.append(
                            pickle.dumps(value, protocol=pk_protocol))
                    else:
                        new_record.append(value)
                else:
                    new_record.append(value)
            return new_record
        else:
            return record
        
    def convert_row(self, row):
        if len(self.table.pickletype_columns):
            new_values = list()
            for column_name, value in zip(row.columns, row.values):
                if value:
                    if self.table.get_column(column_name).is_pickletype:
                        new_values.append(
                            pickle.dumps(value, protocol=pk_protocol))
                    else:
                        new_values.append(value)
                else:
                    new_values.append(value)
            return new_values
        else:
            return row.values
        
    def recover_tuple_record(self, record):
        if len(self.table.pickletype_columns):
            new_record = list()
            for column, value in zip(self.table.all, record):
                if value:
                    if column.is_pickletype:
                        new_record.append(
                            pickle.loads(value))
                    else:
                        new_record.append(value)
                else:
                    new_record.append(value)
            return tuple(new_record)
        else:
            return record

    def recover_list_record(self, record):
        if len(self.table.pickletype_columns):
            new_record = list()
            for column, value in zip(self.table.all, record):
                if value:
                    if column.is_pickletype:
                        new_record.append(
                            pickle.loads(value))
                    else:
                        new_record.append(value)
                else:
                    new_record.append(value)
            return new_record
        else:
            return record
        
    def recover_row(self, record):
        if len(self.table.pickletype_columns):
            column_names = list()
            new_record = list()
            for column, value in zip(self.table.all, record):
                if value:
                    if column.is_pickletype:
                        new_record.append(
                            pickle.loads(value))
                    else:
                        new_record.append(value)
                else:
                    new_record.append(value)
            return Row(columns=self.table.column_names, 
                       values=new_record)
        else:
            return Row(columns=self.table.column_names, values=record)
###############################################################################
#                            Sqlite3Engine class                              #
###############################################################################

class Sqlite3Engine():
    def __init__(self, dbname, autocommit=True):
        self.dbname = dbname
        self.connect = sqlite3.connect(dbname, 
                                       detect_types=sqlite3.PARSE_DECLTYPES)
        self.cursor = self.connect.cursor()
        
        self.set_autocommit(autocommit)
    
    def __str__(self):
        return "Sqlite3Engine(dbname=r'%s', autocommit=%s)" % (
            self.dbname, self.is_autocommit)
    
    def __repr__(self):
        return "Sqlite3Engine(dbname=r'%s', autocommit=%s)" % (
            self.dbname, self.is_autocommit)
    
    def execute(self, *args, **kwarg):
        return self.cursor.execute(*args, **kwarg)
    
    def commit(self):
        """Method for manually commit operation.
        """
        self.connect.commit()

    def commit_nothing(self):
        """Method for doing nothing.
        """
        pass

    def set_autocommit(self, flag):
        """switch on or off autocommit
        """
        if flag:
            self.is_autocommit = True
            self._commit = self.commit
        else:
            self.is_autocommit = False
            self._commit = self.commit_nothing
    
    def convert_record(self, table, record):
        if len(table.pickletype_columns):
            new_record = list()
            for column, value in zip(table.all, record):
                if value:
                    if column.is_pickletype:
                        new_record.append(
                            pickle.dumps(value, protocol=pk_protocol))
                    else:
                        new_record.append(value)
                else:
                    new_record.append(value)
            return new_record
        else:
            return record
        
    def convert_row(self, table, row):
        if len(table.pickletype_columns):
            new_values = list()
            for column_name, value in zip(row.columns, row.values):
                if value:
                    if table.get_column(column_name).is_pickletype:
                        new_values.append(
                            pickle.dumps(value, protocol=pk_protocol))
                    else:
                        new_values.append(value)
                else:
                    new_values.append(value)
            return new_values
        else:
            return row.values
        
    # Execute Insert
    def insert_record(self, ins_obj, record):
        ins_obj.sql_from_record()
        self.execute(ins_obj.sql, self.convert_record(ins_obj.table, record))
        self._commit()
        
    def insert_row(self, ins_obj, row):
        ins_obj.sql_from_row(row)
        self.execute(ins_obj.sql, self.convert_row(ins_obj.table, row))
        self._commit()
    
    def insert_many_record(self, ins_obj, records, cache_size=1024):
        """
        """
        converter = PickleTypeConverter(ins_obj.table) # compile converter
        ins_obj.sql_from_record()
        for record in map(converter.convert_record, records):
            try:
                self.execute(ins_obj.sql, record)
            except:
                pass
        self._commit()

    def insert_many_row(self, ins_obj, rows):
        converter = PickleTypeConverter(ins_obj.table) # compile converter
        ins_obj.sql_from_row(rows[0])
        for record in map(converter.convert_row, rows):
            try:
                self.execute(ins_obj.sql, record)
            except:
                pass
        self._commit()

    def insert_record_stream(self, ins_obj, generator, cache_size=1024):
        for chunk in grouper_list(generator, n=cache_size):
            self.insert_many_record(ins_obj, chunk)

    def insert_row_stream(self, ins_obj, generator, cache_size=1024):
        for chunk in grouper_list(generator, n=cache_size):
            self.insert_many_row(ins_obj, chunk)
    
    # Execute Select
    def select(self, select_obj, return_tuple=False):
        adaptor = PickleTypeConverter(select_obj._temp_table)
        if return_tuple:
            return map(adaptor.recover_tuple_record, 
                       self.cursor.execute(select_obj.sql))
        else:
            return map(adaptor.recover_list_record, 
                       self.cursor.execute(select_obj.sql))
            
    def select_row(self, select_obj):
        adaptor = PickleTypeConverter(select_obj._temp_table)
        return map(adaptor.recover_row, 
                   self.cursor.execute(select_obj.sql))
    
    def select_dict(self, select_obj):
        d = OrderedDict()
        for column_name in select_obj._temp_table.column_names:
            d[column_name] = list()
        for record in self.select(select_obj):
            for column_name, value in zip(
                select_obj._temp_table.column_names, record):
                d[column_name].append(value)
        return d
    
if __name__ == "__main__":
    from sqlite4dummy.dtype import dtype
    from sqlite4dummy.schema import Column, Table, MetaData
    import unittest
    
    class Sqlite3EngineConverterUnittest(unittest.TestCase):
        def setUp(self):
            self.metadata = MetaData()
            self.has_pk = Table("has_pk", self.metadata,
                Column("_id", dtype.TEXT),
                Column("_list", dtype.PICKLETYPE),
                )
            self.no_pk = Table("no_pk", self.metadata,
                Column("_id", dtype.TEXT),
                Column("_value", dtype.INTEGER),
                )
            self.engine = Sqlite3Engine(":memory:")

        def test_single_item_converter(self):
            """测试是否能将数据根据Table中列关于PickleType的定义, 正确转化为
            sqlite3原生API可接受的tuple/list。
            """
            self.assertEqual(self.engine.convert_record(self.has_pk, 
                                                        ("F-001", [1, 2, 3])),
                             ['F-001', b'\x80\x03]q\x00(K\x01K\x02K\x03e.'])
            self.assertEqual(self.engine.convert_row(self.has_pk,
                                                     Row(("_id",), ("F-001",))),
                             ["F-001",])
            self.assertEqual(self.engine.convert_row(self.has_pk,
                             Row(("_id", "_list"), ("F-001", [1, 2, 3]))),
                             ["F-001", b"\x80\x03]q\x00(K\x01K\x02K\x03e."])
            
            self.assertEqual(self.engine.convert_record(self.no_pk, 
                                                        ("F-001", 100)),
                             ('F-001', 100))
            self.assertEqual(self.engine.convert_row(self.no_pk,
                                                     Row(("_id",), ("F-001",))),
                             ("F-001",))
            self.assertEqual(self.engine.convert_row(self.no_pk,
                             Row(("_id", "_value"), ("F-001", 100))),
                             ("F-001", 100))
    unittest.main()