#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
English Doc
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:class:`Sqlite3Engine` is the top class you are doing CRUD with. You could define
the Engine by giving the database file path (for memory database, use 
``":memory:"``).
And this class provides full range of CRUD methods, it's simple, straight forward
like a human language

For all available methods, go :class:`Sqlite3Engine`.

For example usage, click the link, and read the source code link:

- :mod:`Create table <sqlite4dummy.tests.test_MetaData>`
- :mod:`CREATE (Insert) <sqlite4dummy.tests.test_Insert>`
- :mod:`READ (Select) <sqlite4dummy.tests.test_Select>`
- :mod:`Update (Update)<sqlite4dummy.tests.test_Update>`
- :mod:`Delete (Delete) <sqlite4dummy.tests.test_Delete>`
- :mod:`Index <sqlite4dummy.tests.test_Index>`


Chinese Doc (中文文档)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:class:`Sqlite3Engine` 这个类是我们使用 ``·sqlite4dummy`` 时主要所使用的类。他提供
了一系列的简单易懂, 类似人类语言的方法完成我们常用的 "增删查改操作"。

请前往 :class:`Sqlite3Engine` 查看我提供的所有方法。

请前往这里查看所有的使用例子(点击source code按钮即可看到例子源代码)。

- :mod:`建立表 <sqlite4dummy.tests.test_MetaData>`
- :mod:`增 (Insert) <sqlite4dummy.tests.test_Insert>`
- :mod:`删 (Delete) <sqlite4dummy.tests.test_Delete>`
- :mod:`查 (Select) <sqlite4dummy.tests.test_Select>`
- :mod:`改 (Update) <sqlite4dummy.tests.test_Update>`
- :mod:`建立索引 <sqlite4dummy.tests.test_Index>`


class, method, func, exception
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

try:
    from sqlite4dummy.row import Row
except ImportError:
    from .row import Row
    
try:
    from sqlite4dummy.iterate import grouper_list
except ImportError:
    from .iterate import grouper_list
    
try:
    from sqlite4dummy.schema import Select
except ImportError:
    from .schema import Select

from collections import OrderedDict
from datetime import datetime
import sqlite3
import pickle
import logging
import sys
import os

try:
    import pandas as pd
except ImportError:
    print("pandas not found, the select_df feature is not able to work.")

if sys.version_info[0] == 3:
    PK_PROTOCOL = 3
else:
    PK_PROTOCOL = 2

class PickleTypeConverter():
    """High performance PickleType data converter Class.
    
    This is how we handle python type in sqlite database. If I have an entry::
        
        >>> record = (1, [1, 2, 3])
        >>> new_record = convert(record)
        >>> new_record # (1, pickle.dumps([1, 2, 3])
        (1, b'\\x80\\x03]q\\x00(K\\x01K\\x02K\\x03e.')
        
    ``new_record`` is the one we saved in database. When I want to take data out,
    I do it reversely. So basically I am doing this::
    
        picklable Python type <-- convert --> bytes
        
    The way I convert it is defined by the schema of the Table. So 
    :class:`~PickleTypeConverter` takes a :class:`~sqlite4dummy.schema.Table` as
    initialize argument. Then we compile the convert method. Once it is done, 
    then we can make use of built-in high performance vectorize function 
    map(func, iterable).
    
    **中文文档**
    
    用于解决PickleType的读写的数据转换器。
    
    在执行INSERT时, 需要将tuple或row dict转换成sqlite3原生API可接受的tuple, 并
    要保证PickleType正确地被序列化。
    
    在执行Select时, 需要cursor返回的tuple转换成被pickle解码后的Python对象。
    
    在这两个过程中, 需要有所有涉及到的Column的相关信息。我们的做法是在初始化
    PickleTypeConverter时绑定Table, 然后绑定convert方法。这样就可以利用map高性能
    并行处理函数, 批量转换数据。
    """
    def __init__(self, table):
        self.table = table

    def convert_record(self, record):
        """Covert PickleType value in record tuple to Blob.
        
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
                            pickle.dumps(value, protocol=PK_PROTOCOL))
                    else:
                        new_record.append(value)
                else:
                    new_record.append(value)
            return new_record
        else:
            return record
        
    def convert_row(self, row):
        """Covert PickleType value in :class:`~sqlite4dummy.row.Row` object to 
        Blob. Returns list.
        
        :param record: input Row object data.
        :type record: :class:`~sqlite4dummy.row.Row`
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
                            pickle.dumps(value, protocol=PK_PROTOCOL))
                    else:
                        new_values.append(value)
                else:
                    new_values.append(value)
            return new_values
        else:
            return row.values
        
    def recover_tuple_record(self, record):
        """Convert PickleType value in record tuple that naive Python sqlite3 
        API returned to Python object, returns tuple. 
        
        **中文文档**
        
        将原生API cursor.execute("SELECT ...") 所返回的record tuple, 如果其中有
        PickleType, 则转换会Python object。最终返回tuple。
        """
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
        """Convert PickleType value in record tuple that naive Python sqlite3 
        API returned to Python object, returns list. 
        
        **中文文档**
        
        将原生API cursor.execute("SELECT ...") 所返回的record tuple, 如果其中有
        PickleType, 则转换会Python object。最终返回list。
        """
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
        """Convert PickleType value in record tuple that naive Python sqlite3 
        API returned to Python object, returns :class:`~sqlite4dummy.row.Row`. 
        
        **中文文档**
        
        将原生API cursor.execute("SELECT ...") 所返回的record tuple, 如果其中有
        PickleType, 则转换会Python object。最终返回 
        :class:`~sqlite4dummy.row.Row`。
        """
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
            return Row(columns=self.table.column_names, 
                       values=new_record)
        else:
            return Row(columns=self.table.column_names, values=record)
        
###############################################################################
#                            Sqlite3Engine class                              #
###############################################################################

class Sqlite3Engine():
    """A High level API of sqlite3 engine.
    
    **Database Level**:
    
    - :meth:`~Sqlite3Engine.execute`
    - :meth:`~Sqlite3Engine.execute_many`
    - :meth:`~Sqlite3Engine.commit`
    
    **Insert**:
    
    - :meth:`~Sqlite3Engine.insert_record`
    - :meth:`~Sqlite3Engine.insert_row`
    - :meth:`~Sqlite3Engine.insert_many_record`
    - :meth:`~Sqlite3Engine.insert_many_row`
    - :meth:`~Sqlite3Engine.insert_record_stream`
    - :meth:`~Sqlite3Engine.insert_row_stream`
    
    **Select**:
    
    - :meth:`~Sqlite3Engine.select`
    - :meth:`~Sqlite3Engine.select_record`
    - :meth:`~Sqlite3Engine.select_row`
    - :meth:`~Sqlite3Engine.select_dict`
    - :meth:`~Sqlite3Engine.select_df`
    
    **Update**:
    
    - :meth:`~Sqlite3Engine.update`
    - :meth:`~Sqlite3Engine.insdate_many_record`
    - :meth:`~Sqlite3Engine.insdate_many_row`
    
    **Delete**:
    
    - :meth:`~Sqlite3Engine.delete`
    
    **Vanilla method**: sets of syntax sugar methods to reduce the code you need.
    
    - :meth:`~Sqlite3Engine.howmany`
    - :meth:`~Sqlite3Engine.tabulate`
    - :meth:`~Sqlite3Engine.dictize`
    - :meth:`~Sqlite3Engine.to_df`
    - :meth:`~Sqlite3Engine.prt_all`
    - :meth:`~Sqlite3Engine.remove_all`
    
    **Property method**: 
    
    - :meth:`~Sqlite3Engine.all_tablename`
    - :meth:`~Sqlite3Engine.all_indexname`
    """
    def __init__(self, dbname, 
            autocommit=True, echo=False, log=False):
        self.dbname = dbname
        self.connect = sqlite3.connect(
            dbname, detect_types=sqlite3.PARSE_DECLTYPES)
        self.connect.text_factory = str
        
        self.cursor = self.connect.cursor()
        
        self.set_autocommit(autocommit)
        
        self.set_logger(echo, log)
        
    def __str__(self):
        return "Sqlite3Engine(dbname=r'%s', autocommit=%s)" % (
            self.dbname, self.is_autocommit)
    
    def __repr__(self):
        return "Sqlite3Engine(dbname=r'%s', autocommit=%s)" % (
            self.dbname, self.is_autocommit)
    
    def close(self):
        """Close the connection.
        """
        self.connect.close()
    
    def execute(self, sql, *args):
        """Execute SQL command.
        
        **中文文档**
        
        执行原生
        `Cursor.execute <https://docs.python.org/3.3/library/sqlite3.html#sqlite3.Cursor.execute>`_
        方法。
        """
        return self.cursor.execute(sql, *args)

    def executemany(self, sql, *args):
        """Call generic sqlite3 API bulk insert method.
        
        **中文文档**
        
        执行原生
        `Cursor.executemany <https://docs.python.org/3.3/library/sqlite3.html#sqlite3.Cursor.executemany>`_
        方法。
        """
        return self.cursor.executemany(sql, *args)
    
    def commit(self):
        """Method for manually commit operation.
        
        **中文文档**
        
        执行commit。
        """
        self.connect.commit()

    def commit_nothing(self):
        """Method for doing nothing.
        
        **中文文档**
        
        什么都不做。
        """
        pass

    def set_autocommit(self, flag):
        """Switch on or off autocommit.
        
        **中文文档**
        
        设置自动commit开关。
        """
        if flag:
            self.is_autocommit = True
            self._commit = self.commit
        else:
            self.is_autocommit = False
            self._commit = self.commit_nothing
    
    def set_logger(self, echo, log):
        """Switch on or off echo Sql command.
        """
        log_dir = "sqlite4dummy_log"
        log_file = "%s.log" % datetime.strftime(datetime.now(), "%Y-%m-%d_%H-%M-%S.%f")
        log_path = os.path.join(log_dir, log_file)
        
        logger = logging.getLogger("sqlite4dummy")
        
        # Debug level
        logger.setLevel(logging.DEBUG)
        
        # Print screen level
        ch = logging.StreamHandler()
        if echo: # print message only higher than INFO
            ch.setLevel(logging.INFO)
        else: # print message only higher than WARNING
            ch.setLevel(logging.WARNING)
        logger.addHandler(ch)
        
        # File and format
        if log:
            try:
                os.mkdir(log_dir)
            except:
                pass
            fh = logging.FileHandler(log_path)
            formatter = logging.Formatter("[%(asctime)s][%(name)s][%(levelname)s][%(message)s]")
            fh.setFormatter(formatter)
            logger.addHandler(fh)
        self.logger = logger
        
    # non-compiled version of pickle record/row converter
    def convert_record(self, table, record):
        """Non-compiled version of pickle record converter.
        
        **中文文档**
        
        :meth:`PickleTypeConverter.convert_record` 的类似版本, 用于只执行一次的
        insert和update的情况。
        """
        if len(table.pickletype_columns):
            new_record = list()
            for column, value in zip(table.all, record):
                if value:
                    if column.is_pickletype:
                        new_record.append(
                            pickle.dumps(value, protocol=PK_PROTOCOL))
                    else:
                        new_record.append(value)
                else:
                    new_record.append(value)
            return new_record
        else:
            return record
        
    def convert_row(self, table, row):
        """Non-compiled version of pickle row converter.
        
        **中文文档**
        
        :meth:`PickleTypeConverter.convert_row` 的类似版本, 用于只执行一次的
        insert和update的情况。
        """
        if len(table.pickletype_columns):
            new_values = list()
            for column_name, value in zip(row.columns, row.values):
                if value:
                    if table.get_column(column_name).is_pickletype:
                        new_values.append(
                            pickle.dumps(value, protocol=PK_PROTOCOL))
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
        
        :param ins_obj: :class:`~sqlite4dummy.schema.Insert` object
        :type ins_obj: :class:`~sqlite4dummy.schema.Insert`
        
        :param record: tuple data.
        :type record: tuple
        
        **中文文档**
        
        插入单条tuple或list数据。
        """
        ins_obj.sql_from_record()
        self.cursor.execute(ins_obj.sql, self.convert_record(
                                            ins_obj.table, record))
        self._commit()
        
    def insert_row(self, ins_obj, row):
        """Insert single Row.
        
        :param ins_obj: :class:`~sqlite4dummy.schema.Insert` object
        :type ins_obj: :class:`~sqlite4dummy.schema.Insert`
        
        :param row: :class:`~sqlite4dummy.row.Row` data.
        :type row: :class:`~sqlite4dummy.row.Row`
        
        **中文文档**
        
        插入单条 :class:`~sqlite4dummy.row.Row` 数据。
        """
        ins_obj.sql_from_row(row)
        self.cursor.execute(ins_obj.sql, self.convert_row(
                                            ins_obj.table, row))
        self._commit()
    
    def insert_many_record(self, ins_obj, records):
        """Insert many records, skip all primary-key conflict data.
        
        :param ins_obj: :class:`~sqlite4dummy.schema.Insert` object
        :type ins_obj: :class:`~sqlite4dummy.schema.Insert`
        
        :param generator: data stream
        :type generator: generator
        
        **中文文档**
        
        插入多条tuple或list数据。
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
        
        :param ins_obj: :class:`~sqlite4dummy.schema.Insert` object
        :type ins_obj: :class:`~sqlite4dummy.schema.Insert`
        
        :param generator: data stream
        :type generator: generator
        
        **中文文档**
        
        插入多条 :class:`~sqlite4dummy.row.Row` 数据。
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
        """Another version of :meth:`~Sqlite3Engine.insert_many_record`, take
        generator type input data stream.
        
        :param ins_obj: :class:`~sqlite4dummy.schema.Insert` object
        :type ins_obj: :class:`~sqlite4dummy.schema.Insert`
        
        :param generator: data stream
        :type generator: generator
        
        :param cache_size: Execute how many data one at a time.
        :type cache_size: int
        
        **中文文档**
        
        以生成器形式插入多条tuple或list数据。
        """
        for chunk in grouper_list(generator, n=cache_size):
            self.insert_many_record(ins_obj, chunk)
        self._commit()
        
    def insert_row_stream(self, ins_obj, generator, cache_size=1024):
        """Another version of :meth:`~Sqlite3Engine.insert_many_row`, take
        generator type input data stream.
        
        :param ins_obj: :class:`~sqlite4dummy.schema.Insert` object
        :type ins_obj: :class:`~sqlite4dummy.schema.Insert`
        
        :param generator: data stream
        :type generator: generator
        
        :param cache_size: Execute how many data one at a time.
        :type cache_size: int
        
        **中文文档**
        
        以生成器的形式插入单条 :class:`~sqlite4dummy.row.Row` 数据。
        """
        for chunk in grouper_list(generator, n=cache_size):
            self.insert_many_row(ins_obj, chunk)
        self._commit()
        
    # Execute Select    
    def select(self, sel_obj, return_tuple=False):
        """Execute :class:`~sqlite4dummy.schema.Select` object, 
        if ``return_tuple=True``, yield ``tuple``, else, yield ``list``.
        
        **中文文档**
        
        执行 :class:`~sqlite4dummy.schema.Select` 对象, 返回tuple数据
        """
        self.logger.info(sel_obj.sql)
        adaptor = PickleTypeConverter(sel_obj.temp_table)
        if return_tuple:
            return map(adaptor.recover_tuple_record, 
                       self.cursor.execute(sel_obj.sql))
        else:
            return map(adaptor.recover_list_record, 
                       self.cursor.execute(sel_obj.sql))
    
    def select_record(self, sel_obj, return_tuple=False):
        """Alias of :meth:`~Sqlite3Engine.select`
        
        **中文文档**
        
        :meth:`~Sqlite3Engine.select` 的同功能方法。
        """
        return self.select(sel_obj, return_tuple)
    
    def select_row(self, sel_obj):
        """Execute :class:`~sqlite4dummy.schema.Select` object, 
        yield :class:`~sqlite4dummy.row.Row` object.
        
        
        **中文文档**
        
        执行 :class:`~sqlite4dummy.schema.Select` 对象, 返回
        :class:`~sqlite4dummy.row.Row` 数据。
        """
        adaptor = PickleTypeConverter(sel_obj.temp_table)
        return map(adaptor.recover_row, 
                   self.cursor.execute(sel_obj.sql))
    
    def select_dict(self, sel_obj):
        """Execute :class:`~sqlite4dummy.schema.Select` object, 
        returns column oriented view of 2d-DataFrame.
        
        Example returns::
    
            {
                "column1": [value1, value2, ...],
                "column2": [value1, value2, ...],
                ...,
                "columnN": [value1, value2, ...],
            }
            
        **中文文档**
        
        执行 :class:`~sqlite4dummy.schema.Select` 对象, 返回以列为导向的字典视图。
        """
        d = OrderedDict()
        for column_name in sel_obj.temp_table.column_names:
            d[column_name] = list()
        for record in self.select(sel_obj):
            for column_name, value in zip(
                sel_obj.temp_table.column_names, record):
                d[column_name].append(value)
        return d
    
    def select_df(self, sel_obj):
        """Execute :class:`~sqlite4dummy.schema.Select` object, 
        returns 
        `pandas.DataFrame <http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.html>`_ 
        column oriented view. Faster than :meth:`~Sqlite3Engine.select_dict`.
        
        **中文文档**
        
        执行 :class:`~sqlite4dummy.schema.Select` 对象, 返回
        `pandas.DataFrame <http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.html>`_
        数据。
        """
        return pd.DataFrame(
            list(self.select(sel_obj)),
            columns=sel_obj.temp_table.column_names,
            )
    
    # Execute Update
    def update(self, upd_obj):
        """Execute :class:`~sqlite4dummy.schema.Update` object.
        
        **中文文档**
        
        执行 :class:`~sqlite4dummy.schema.Update` 对象。
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
        """Another version taking :class:`~sqlite4dummy.row.Row` object data.
        
        **中文文档**
        
        :meth:`~Sqlite3Engine.insdate_many_record` 的同功能方法, 只不过接受的是
        :class:`~sqlite4dummy.row.Row` 数据。
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
        """Execute :class:`~sqlite4dummy.schema.Delete` object.
        
        **中文文档**
        
        执行 :class:`~sqlite4dummy.schema.Delete` 对象。
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
        except sqlite3.OperationalError as e:
            raise e
        except:
            print("Argument has to be a Table object or a table name.")
            raise
            
    def drop_index(self, index):
        """Drop an index by Index object (or by index name).
    
        **中文文档**
        
        删除某个索引。接受Index对象或index name字符串。
        """
        try:
            self.execute("DROP INDEX %s" % index)
            index.metadata._remove_index(index)
        except sqlite3.OperationalError as e:
            raise e
        except:
            print("Argument has to be an Index object or an index name.")
            raise
        
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
        except sqlite3.OperationalError as e:
            raise e
        except:
            print("Argument has to be a Table object or a table name.")
            raise
        
    def tabulate(self, table):
        """Return all data in a table in list of records format.
        
        **中文文档**
        
        以**list of list**的形式返回表中**所有**数据
        """
        return list(self.select(Select(table.all)))
    
    def dictize(self, table):
        """Return all data in a table in json like, column oriented format.
        
        **中文文档**
        
        以**字典视图**的形式返回表中**所有**数据
        """
        return self.select_dict(Select(table.all))
    
    def to_df(self, table):
        """Return all data and wrapped into a pandas.DataFrame object.
        Faster than :meth:`~Sqlite3Engine.dictize`.
        
        **中文文档**
        
        以**pandas.DataFrame**的形式返回表中**所有**数据
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

    def remove_all(self, table):
        """Remove all data in a table by Table object (or by table name).
        
        **中文文档**
        
        删除某个表中的数据。接受Table对象或table name字符串。
        """
        try:
            self.execute("DELETE FROM %s" % table)
            self._commit()
            print("All data in %s has been removed, (index is keeped)" % table)
        except sqlite3.OperationalError as e:
            raise e
            
    @property
    def all_tablename(self):
        """Returns list of table name in this database.
        
        **中文文档**
        
        返回数据库中的所有表名的list。
        """
        tablename_list = list()
        for record in self.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"):
            tablename_list.append(record[0])
        return tablename_list
    
    @property
    def all_indexname(self):
        """Returns list of index name in this database.
        
        **中文文档**
        
        返回数据库中的所有索引名的list。
        """
        indexname_list = list()
        for record in self.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'index' AND sql NOT NULL;"):
            indexname_list.append(record[0])
        return indexname_list
    
if __name__ == "__main__":
    from sqlite4dummy import *
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
                             ['F-001', pickle.dumps([1, 2, 3], protocol=PK_PROTOCOL)])
            self.assertEqual(self.engine.convert_row(self.has_pk,
                                                     Row(("_id",), ("F-001",))),
                             ["F-001",])
            self.assertEqual(self.engine.convert_row(self.has_pk,
                             Row(("_id", "_list"), ("F-001", [1, 2, 3]))),
                             ["F-001", pickle.dumps([1, 2, 3], protocol=PK_PROTOCOL)])
             
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
            """测试根据Table编译converter的过程是否成功。
            """
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
            self.assertEqual(self.engine.howmany(self.table), total)
              
        def test_tabulate(self):
            self.assertEqual(len(self.engine.tabulate(self.table)), total)
                
        def test_dictize(self):
            self.assertEqual(len(self.engine.dictize(self.table)), 8)
            self.assertEqual(len(self.engine.dictize(self.table)["_id"]), total)
                
        def test_prt_all(self):
            self.engine.prt_all(self.table)
            print(self.engine.tabulate(self.table))
            print(self.engine.dictize(self.table))
           
        def test_remove_all(self):
            self.engine.remove_all("test")
               
        def test_property_method(self):
            self.assertEqual(self.engine.all_tablename, ["test"])
            self.assertEqual(self.engine.all_indexname, [])
            
    unittest.main()