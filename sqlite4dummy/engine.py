#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
English Doc
~~~~~~~~~~~

TODO


Chinese Doc (中文文档)
~~~~~~~~~~~~~~~~~~~~~~



class, method, func, exception
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

from sqlite4dummy.row import Row
from sqlite4dummy.iterate import grouper_list
from sqlite4dummy.schema import Select
from collections import OrderedDict
import sqlite3
import pickle
import sys

try:
    import pandas as pd
except ImportError:
    print("pandas not found, the select_df feature is not able to work.")


is_py2 = (sys.version_info[0] == 2)
if is_py2:
    pk_protocol = 2
else:
    pk_protocol = 3

class PickleTypeConverter():
    """Compiled PickleType field converter object.
    
    Once the converted been compiled, then we can use built-in high performance 
    batch process function map(func, iterable).
    
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
        """
        
        :param record: input tuple data.
        :type record: tuple or list
        :return: list that pickletype fields been converted. 
        
        **中文文档**
        
        将record中的pickletype的项转化成blob。该方法用于在Insert操作之前对数据
        进行预处理。返回list。
        """
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
        """
        
        :param record: input Row object data.
        :type record: :class:`sqlite4dummy.row.Row`
        :return: list that pickletype fields been converted. 
        
        **中文文档**
        
        将Row对象中的pickletype的项转化成blob。该方法用于在Insert操作之前对数据
        进行预处理。返回list。
        """
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
    """A top API of sqlite3 engine.
    """
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
        """Execute sql command.
        """
        return self.cursor.execute(*args, **kwarg)

    def executemany(self, *args, **kwarg):
        """Call generic sqlite3 API bulk insert method.
        """
        return self.cursor.executemany(*args, **kwarg)
    
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
    
    # non-compiled version of pickle record/row converter
    def convert_record(self, table, record):
        """non-compiled version of pickle record converter
        """
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
        """non-compiled version of pickle row converter
        """
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
        """Insert single record.
        
        :param ins_obj: :class:`Insert<sqlite4dummy.schema.Insert>` object
        :type ins_obj: :class:`Insert<sqlite4dummy.schema.Insert>`
        
        :param record: tuple data.
        :type record: tuple
        """
        ins_obj.sql_from_record()
        self.cursor.execute(ins_obj.sql, self.convert_record(
                                            ins_obj.table, record))
        self._commit()
        
    def insert_row(self, ins_obj, row):
        """Insert single Row.
        
        :param ins_obj: :class:`Insert<sqlite4dummy.schema.Insert>` object
        :type ins_obj: :class:`Insert<sqlite4dummy.schema.Insert>`
        
        :param row: :class:`Row<sqlite4dummy.row.Row>` data.
        :type row: :class:`Row<sqlite4dummy.row.Row>`
        """
        ins_obj.sql_from_row(row)
        self.cursor.execute(ins_obj.sql, self.convert_row(
                                            ins_obj.table, row))
        self._commit()
    
    def insert_many_record(self, ins_obj, records):
        """Insert many records, skip all primary-key conflict data.
        
        :param ins_obj: :class:`Insert<sqlite4dummy.schema.Insert>` object
        :type ins_obj: :class:`Insert<sqlite4dummy.schema.Insert>`
        
        :param generator: data stream
        :type generator: generator
        """
        converter = PickleTypeConverter(ins_obj.table) # compile converter
        ins_obj.sql_from_record()
        for record in map(converter.convert_record, records):
            try:
                self.cursor.execute(ins_obj.sql, record)
            except:
                pass
        self._commit()

    def insert_many_row(self, ins_obj, rows):
        """Insert many Row, skip all primary-key conflict data.
        
        :param ins_obj: :class:`Insert<sqlite4dummy.schema.Insert>` object
        :type ins_obj: :class:`Insert<sqlite4dummy.schema.Insert>`
        
        :param generator: data stream
        :type generator: generator
        """
        converter = PickleTypeConverter(ins_obj.table) # compile converter
        ins_obj.sql_from_row(rows[0])
        for record in map(converter.convert_row, rows):
            try:
                self.cursor.execute(ins_obj.sql, record)
            except:
                pass
        self._commit()

    def insert_record_stream(self, ins_obj, generator, cache_size=1024):
        """Another version of :meth:`Sqlite3Engine.insert_many_record`, take
        generator type input data stream.
        
        :param ins_obj: :class:`Insert<sqlite4dummy.schema.Insert>` object
        :type ins_obj: :class:`Insert<sqlite4dummy.schema.Insert>`
        
        :param generator: data stream
        :type generator: generator
        
        :param cache_size: Execute how many data one at a time.
        :type cache_size: int
        """
        for chunk in grouper_list(generator, n=cache_size):
            self.insert_many_record(ins_obj, chunk)
        self._commit()
        
    def insert_row_stream(self, ins_obj, generator, cache_size=1024):
        """Another version of :meth:`Sqlite3Engine.insert_many_row`, take
        generator type input data stream.
        
        :param ins_obj: :class:`Insert<sqlite4dummy.schema.Insert>` object
        :type ins_obj: :class:`Insert<sqlite4dummy.schema.Insert>`
        
        :param generator: data stream
        :type generator: generator
        
        :param cache_size: Execute how many data one at a time.
        :type cache_size: int
        """
        for chunk in grouper_list(generator, n=cache_size):
            self.insert_many_row(ins_obj, chunk)
        self._commit()
        
    # Execute Select
    def select(self, sel_obj, return_tuple=False):
        """Execute :class:`Select<sqlite4dummy.schema.Select>` object, 
        if ``return_tuple=True``, yield ``tuple``, else, yield ``list``.
        """
        adaptor = PickleTypeConverter(sel_obj._temp_table)
        if return_tuple:
            return map(adaptor.recover_tuple_record, 
                       self.cursor.execute(sel_obj.sql))
        else:
            return map(adaptor.recover_list_record, 
                       self.cursor.execute(sel_obj.sql))
    
    def select_record(self, sel_obj, return_tuple=False):
        """Alias of :meth:`Sqlite3Engine.select`
        """
        return self.select(sel_obj, return_tuple)
    
    def select_row(self, sel_obj):
        """Execute :class:`Select<sqlite4dummy.schema.Select>` object, 
        yield :class:`Row<sqlite4dummy.row.Row>` object.
        """
        adaptor = PickleTypeConverter(sel_obj._temp_table)
        return map(adaptor.recover_row, 
                   self.cursor.execute(sel_obj.sql))
    
    def select_dict(self, sel_obj):
        """Execute :class:`Select<sqlite4dummy.schema.Select>` object, 
        returns column oriented view of 2d-DataFrame.
        
        Example returns::
    
            {
                "column1": [value1, value2, ...],
                "column2": [value1, value2, ...],
                ...,
                "columnN": [value1, value2, ...],
            }
        """
        d = OrderedDict()
        for column_name in sel_obj._temp_table.column_names:
            d[column_name] = list()
        for record in self.select(sel_obj):
            for column_name, value in zip(
                sel_obj._temp_table.column_names, record):
                d[column_name].append(value)
        return d
    
    def select_df(self, sel_obj):
        """Execute :class:`Select<sqlite4dummy.schema.Select>` object, 
        returns pandas.DataFrame column oriented view. Faster than 
        :meth:`Sqlite3Engine.select_dict`.
        """
        return pd.DataFrame(
            list(self.select(sel_obj)),
            columns=sel_obj._temp_table.column_names,
            )
    
    # Execute Update
    def update(self, upd_obj):
        """Execute :class:`Update<sqlite4dummy.schema.Update>` object.
        """
        self.cursor.execute(upd_obj.sql)
        self._commit()
    
    # Execute Insdate
    def insdate_many_record(self, ins_obj, records):
        """INSDATE (insert or update), batch insert and update records.
        
        If meet sql integrity error, then locate to the record via primary key,
        and update it. Here's an example.
        
        First we try::
        
            INSERT INTO test
            (_id, content)
            VALUES 
            (1, 'hello world')
        
        Failed, then we do::
        
            UPDATE test
            SET content = 'hello world'
            WHERE _id = 1
            
        **中文文档**
        
        智能插入和更新。
        
        在尝试插入一条记录时, 如果字段中包括Primary Key, 那么可能出现
        Integrity Error, 一旦发生冲突, 则使用WHERE主键Key定位到条目, 然后Update
        其他字段::
        
            INSERT INTO test
            (_id, content)
            VALUES 
            (1, 'hello world')
        
        冲突! 进行更新::
        
            UPDATE test
            SET content = 'hello world'
            WHERE _id = 1
        
        如果字段中不包括Primary Key, 则肯定能Insert成功。
        """
        upd_obj = ins_obj.table.update()
        ins_obj.sql_from_record()
        
        for record in records: # try insert one by one
            try: # try insert
                self.cursor.execute(ins_obj.sql, self.convert_record(
                                                    ins_obj.table, record))
            except sqlite3.IntegrityError:
                values_kwarg = dict() # update.values() argument
                where_args = list() # update.values().where() argument
                 
                for column_name, column, value in zip(
                    upd_obj.table.column_names,
                    upd_obj.table.all,
                    record,
                    ):
                    if column.primary_key: # use primary_key value to match row
                        where_args.append( column == value)
                    else:
                        values_kwarg[column_name] = value # fill in values kwarg
                # update one
                if len(values_kwarg) >= 1:
                    self.update(upd_obj.\
                                values(**values_kwarg).\
                                where(*where_args))
            except Exception as e:
                print("Error message: %s" % e)
                
        self._commit()
        
    def insdate_many_row(self, ins_obj, rows):
        """Another version taking :class:`Row<sqlite4dummy.row.Row>` object data.
        """
        upd_obj = ins_obj.table.update()
        
        for row in rows: # try insert one by one
            try: # try insert
                ins_obj.sql_from_row(row)
                self.cursor.execute(ins_obj.sql, self.convert_row(
                                                    ins_obj.table, row))
                
            except sqlite3.IntegrityError:
                values_kwarg = dict() # update.values() argument
                where_args = list() # update.values().where() argument
                
                for column_name, value in zip(row.columns, row.values):
                    column = upd_obj.table.get_column(column_name)
                    if column.primary_key: # use primary_key value to match row
                        where_args.append( column == value)
                    else:
                        values_kwarg[column_name] = value # fill in values kwarg
                
                # update one
                if len(values_kwarg) >= 1:
                    self.update(upd_obj.\
                                values(**values_kwarg).\
                                where(*where_args))
            except Exception as e:
                print("Error message: %s" % e)
                
        self._commit()
        
    # Execute Delete
    def delete(self, del_obj):
        """Execute :class:`Delete<sqlite4dummy.schema.Delete>` object.
        """
        self.cursor.execute(del_obj.sql)
        self._commit()
        
    # Drop TABLE, INDEX command aliase
    # CREATE TABLE, INDEX operation can only performed by creating 
    # Table(), Index() objects, and call the create(engine) method.
    def drop_table(self, table):
        """Drop a table by Table object (or by table name).
        
        **中文文档**
        
        删除某个表的所有数据, 结构, 索引。接受Table对象或table name字符串。
        """
        try:
            self.execute("DROP TABLE %s" % table)
            table.metadata._remove_table(table)
        except Exception as e:
            print(e)
            
    def drop_index(self, index):
        """Drop an index by Index object (or by index name).
    
        **中文文档**
        
        删除某个索引。接受Index对象或index name字符串。
        """
        try:
            self.execute("DROP INDEX %s" % index)
            index.metadata._remove_index(index)
        except Exception as e:
            print(e)
    
    def delete_table(self, table):
        """Delete all data in a table by Table object (or by table name).
        
        **中文文档**
        
        删除某个表中的数据。接受Table对象或table name字符串。
        """
        try:
            self.execute("DELETE FROM %s" % table)
            self._commit()
        except Exception as e:
            print(e)
        
    # Vanilla method
    def howmany(self, table):
        """Returns how many records in a table.
        
        :param table: Represent the table you want to count with.
        :type table: Table object or string
        
        **中文文档**
        
        返回表中有多少条记录。
        """
        try:
            return self.execute("SELECT COUNT(*) FROM (SELECT * FROM %s)" % table).\
                fetchone()[0]
        except:
            Exception("Argument has to be a Table object or a table name.")
    
    def tabulate(self, table):
        """Return all data in a table in list of records format.
        """
        return list(self.select(Select(table.all)))
    
    def dictize(self, table):
        """Return all data in a table in json like, column oriented format.
        """
        return self.select_dict(Select(table.all))
    
    def to_df(self, table):
        """Return all data and wrapped into a pandas.DataFrame object.
        Faster than :meth:`Sqlite3Engine.dictize`.
        """
        return self.select_df(Select(table.all))
    
    def prt_all(self, table):
        """Print all records in a table.
        
        :param table: The Table object.
        :type table: Table object
        
        **中文文档**
        
        打印表中所有的记录。
        """
        print("{:=^100}".format("Select * FROM %s" % table))
        counter = 0
        for record in self.select(Select(table.all)):
            counter += 1
            print(record)
        print("%s records returns" % counter)
    
    @property
    def all_tablename(self):
        tablename_list = list()
        for record in self.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"):
            tablename_list.append(record[0])
        return tablename_list
    
    @property
    def all_indexname(self):
        indexname_list = list()
        for record in self.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'index' AND sql NOT NULL;"):
            indexname_list.append(record[0])
        return indexname_list
    
if __name__ == "__main__":
    from sqlite4dummy.dtype import dtype
    from sqlite4dummy.schema import Column, Table, MetaData
    from sqlite4dummy.tests.test_database_setting import *
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
            """使用engine自带的, 用于处理单次record或row转换的``convert_record()``
            和``convert_row``方法, 根据Table中每列关于PickleType的定义, 将数据中
            是PickleType的项转换为BLOB。这样sqlite3原生API即可接受。
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
            
        def test_compiled_item_converter(self):
            conv_has_pk = PickleTypeConverter(self.has_pk)
            conv_no_pk = PickleTypeConverter(self.no_pk)
            pass
        
    class EngineVanillaMethodUnittest(unittest.TestCase):
        """测试Sqlite3Engine的魔术方法。
        """
        def setUp(self):
            (
                self.metadata,
                self.table,
                self.engine
            ) = initial_all_dtype_database(needdata=True)
        
        def test_howmany(self):
            self.assertEqual(self.engine.howmany(self.table), 1000)
            
        def test_tabulate(self):
            self.assertEqual(len(self.engine.tabulate(self.table)), 1000)
            
        def test_dictize(self):
            self.assertEqual(len(self.engine.dictize(self.table)), 8)
            self.assertEqual(len(self.engine.dictize(self.table)["_id"]), 1000)
            
        def test_prt_all(self):
            self.engine.prt_all(self.table)
#             print(self.engine.tabulate(self.table))
#             print(self.engine.dictize(self.table))
            
                
    unittest.main()